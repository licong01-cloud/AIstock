"""Durable MiniQMT execution runtime domain models.

Phase 2 intentionally keeps these models broker-interface oriented and does not
connect to a production MiniQMT process. The runtime owns event ordering,
gateway calls, OMS projection, and restart recovery.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.trading_core.models import OrderSide


class MiniQMTExecutionRuntimeMode(str, Enum):
    SIM = "SIM"
    LIVE_PENDING_APPROVAL = "LIVE_PENDING_APPROVAL"
    LIVE = "LIVE"


class MiniQMTExecutionRuntimeState(str, Enum):
    CREATED = "CREATED"
    RECOVERING = "RECOVERING"
    READY = "READY"
    RUNNING = "RUNNING"
    RECONCILING = "RECONCILING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class MiniQMTGatewayState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTED = "CONNECTED"
    DEGRADED = "DEGRADED"


class MiniQMTOmsState(str, Enum):
    EMPTY = "EMPTY"
    OPEN = "OPEN"
    RECONCILED = "RECONCILED"
    FAILED = "FAILED"


class MiniQMTExecutionEventType(str, Enum):
    RUNTIME_CREATED = "RUNTIME_CREATED"
    GATEWAY_CONNECTED = "GATEWAY_CONNECTED"
    BROKER_SYNC_STARTED = "BROKER_SYNC_STARTED"
    BROKER_SYNCED = "BROKER_SYNCED"
    ALGO_INSTANCE_CREATED = "ALGO_INSTANCE_CREATED"
    TIMER = "TIMER"
    TICK = "TICK"
    ALGO_ACTION_EMITTED = "ALGO_ACTION_EMITTED"
    CHILD_ORDER_SUBMITTED = "CHILD_ORDER_SUBMITTED"
    CHILD_ORDER_REJECTED = "CHILD_ORDER_REJECTED"
    CHILD_ORDER_CANCEL_REQUESTED = "CHILD_ORDER_CANCEL_REQUESTED"
    ORDER_EVENT = "ORDER_EVENT"
    TRADE_EVENT = "TRADE_EVENT"
    RECONCILE_STARTED = "RECONCILE_STARTED"
    RECONCILE_COMPLETED = "RECONCILE_COMPLETED"
    OPERATOR_COMMAND_RECEIVED = "OPERATOR_COMMAND_RECEIVED"
    RUNTIME_STOPPED = "RUNTIME_STOPPED"


class MiniQMTAlgoInstanceStatus(str, Enum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class MiniQMTChildOrderStatus(str, Enum):
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


def new_runtime_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


class MiniQMTExecutionRuntimeConfig(BaseModel):
    """Stable identity and policy hash for one runtime instance."""

    model_config = ConfigDict(extra="forbid")

    runtime_id: str = Field(default_factory=lambda: new_runtime_id("mqrt"))
    account_group_id: str
    trade_date: date
    mode: MiniQMTExecutionRuntimeMode = MiniQMTExecutionRuntimeMode.SIM
    runtime_config_hash: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("runtime_id", "account_group_id", "runtime_config_hash")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value


class MiniQMTExecutionRuntimeRecord(BaseModel):
    """Durable runtime state projection rebuildable after process restart."""

    model_config = ConfigDict(extra="forbid")

    runtime_id: str
    account_group_id: str
    trade_date: date
    mode: MiniQMTExecutionRuntimeMode = MiniQMTExecutionRuntimeMode.SIM
    event_loop_state: MiniQMTExecutionRuntimeState = MiniQMTExecutionRuntimeState.CREATED
    gateway_state: MiniQMTGatewayState = MiniQMTGatewayState.DISCONNECTED
    oms_state: MiniQMTOmsState = MiniQMTOmsState.EMPTY
    runtime_config_hash: str
    last_event_sequence: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("runtime_id", "account_group_id", "runtime_config_hash")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value


class MiniQMTExecutionEvent(BaseModel):
    """Append-only runtime event with monotonic per-runtime sequence."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: new_runtime_id("mqrtevt"))
    runtime_id: str
    sequence: int = Field(ge=1)
    event_type: MiniQMTExecutionEventType
    event_time: datetime = Field(default_factory=lambda: datetime.now(UTC))
    source: Literal["runtime", "gateway", "oms", "algo", "operator", "recovery"]
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_id", "runtime_id", "source")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value


class MiniQMTExecutionAlgoInstance(BaseModel):
    """Runtime-owned algo instance; Phase 3 will attach vn.py-derived behavior."""

    model_config = ConfigDict(extra="forbid")

    algo_instance_id: str = Field(default_factory=lambda: new_runtime_id("mqalgo"))
    runtime_id: str
    parent_intent_id: str
    strategy_slot_id: str
    symbol: str
    side: OrderSide
    target_quantity: int = Field(gt=0)
    remaining_quantity: int = Field(ge=0)
    algo_code: str
    status: MiniQMTAlgoInstanceStatus = MiniQMTAlgoInstanceStatus.ACTIVE
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("algo_instance_id", "runtime_id", "parent_intent_id", "strategy_slot_id", "symbol", "algo_code")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @model_validator(mode="after")
    def _remaining_does_not_exceed_target(self) -> "MiniQMTExecutionAlgoInstance":
        if self.remaining_quantity > self.target_quantity:
            raise ValueError("remaining_quantity cannot exceed target_quantity")
        return self


class MiniQMTChildOrder(BaseModel):
    """Broker child order emitted by a runtime-owned algo instance."""

    model_config = ConfigDict(extra="forbid")

    child_order_id: str = Field(default_factory=lambda: new_runtime_id("mqchild"))
    runtime_id: str
    algo_instance_id: str
    parent_intent_id: str
    strategy_slot_id: str
    symbol: str
    side: OrderSide
    quantity: int = Field(gt=0)
    price: float = Field(ge=0)
    price_type: int = 11
    status: MiniQMTChildOrderStatus = MiniQMTChildOrderStatus.SUBMITTING
    broker_order_id: str | None = None
    submitted_at: datetime | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "child_order_id",
        "runtime_id",
        "algo_instance_id",
        "parent_intent_id",
        "strategy_slot_id",
        "symbol",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @field_validator("broker_order_id")
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None


class MiniQMTRuntimeRecoverySnapshot(BaseModel):
    """Process restart snapshot reconstructed from durable runtime state."""

    model_config = ConfigDict(extra="forbid")

    runtime: MiniQMTExecutionRuntimeRecord
    events: list[MiniQMTExecutionEvent]
    active_algo_instances: list[MiniQMTExecutionAlgoInstance]
    active_child_orders: list[MiniQMTChildOrder]
    broker_orders: list[dict[str, Any]] = Field(default_factory=list)
    broker_trades: list[dict[str, Any]] = Field(default_factory=list)
    broker_positions: list[dict[str, Any]] = Field(default_factory=list)
    recovered_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @property
    def broker_synced_before_new_orders(self) -> bool:
        start_indexes = [
            index
            for index, event in enumerate(self.events)
            if event.event_type == MiniQMTExecutionEventType.BROKER_SYNC_STARTED
        ]
        start_index = start_indexes[-1] if start_indexes else -1
        for event in self.events[start_index + 1 :]:
            if event.event_type == MiniQMTExecutionEventType.BROKER_SYNCED:
                return True
            if event.event_type == MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED:
                return False
        return False
