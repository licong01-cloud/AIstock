"""Broker-neutral identities, handles, and adapter contract.

This module deliberately owns no order execution, ledger mutation, selection,
or market-data loading.  Concrete economic writers stay in their current
runtime until SIM-LR-B.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import Callable, Literal

from pydantic import BaseModel, ConfigDict, Field

from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.trading_core.models import OrderIntent, PositionLot


BackendId = Literal["local_sim", "minqmt_sim", "minqmt_live"]
BrokerBackendId = Literal["local_sim", "minqmt_sim"]


class OrderHandle(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    handle_id: str
    backend_id: BackendId
    submitted_at: datetime
    intent_id: str


OrderHandleStatusState = Literal[
    "pending",
    "partial_filled",
    "filled",
    "cancelled",
    "rejected",
]


class OrderHandleStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    handle_id: str
    state: OrderHandleStatusState
    filled_quantity: int = Field(ge=0)
    avg_fill_price: Decimal | None = None
    last_event_at: datetime
    rejection_reason: str | None = None
    raw_status: int | str | None = None
    status_msg: str | None = None
    raw: dict[str, object] = Field(default_factory=dict)


class FillEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    handle_id: str
    intent_id: str
    fill_quantity: int = Field(gt=0)
    fill_price: Decimal
    fill_ts: datetime
    venue: str


class BrokerAccountSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend_id: BackendId
    cash: Decimal
    nav: Decimal
    margin_used: Decimal | None = None
    as_of: datetime


class CancelAck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    handle_id: str
    accepted: bool
    reason: str | None = None


class BrokerBindCapacity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend_id: BackendId
    max_concurrent_packages: int = Field(ge=1)
    rejection_reason_if_exceeded: str


class SubscriptionHandle(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    subscription_id: str
    backend_id: BackendId


MarketDataChannelKind = Literal[
    "in_process_tdx",
    "in_process_db",
    "minqmt_xtdata",
]


class MarketDataChannel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    backend_id: BackendId
    source: MinuteDataSource
    channel_kind: MarketDataChannelKind


class BrokerBackend(ABC):
    """Backend-neutral adapter contract; never an economic writer itself."""

    backend_id: BackendId
    backend_version: str

    @abstractmethod
    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle: ...

    @abstractmethod
    def cancel(self, handle: OrderHandle) -> CancelAck: ...

    @abstractmethod
    def query_status(self, handle: OrderHandle) -> OrderHandleStatus: ...

    @abstractmethod
    def subscribe_fill_callback(self, cb: Callable[[FillEvent], None]) -> SubscriptionHandle: ...

    @abstractmethod
    def unsubscribe_fill_callback(self, handle: SubscriptionHandle) -> None: ...

    @abstractmethod
    def query_account(self) -> BrokerAccountSnapshot: ...

    @abstractmethod
    def query_positions(self) -> dict[str, PositionLot]: ...

    def query_quote(self, symbol: str) -> dict[str, object] | None:
        return None

    @abstractmethod
    def market_data_channel(self) -> MarketDataChannel: ...

    @abstractmethod
    def bind_capacity(self) -> BrokerBindCapacity: ...
