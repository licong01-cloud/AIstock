"""BrokerBackend abstract base + supporting Pydantic models.

Strategy Engine design 2026-05-08 §3.6.1 (R-Q9 D1). Schema decisions confirmed
by Lead 2026-05-08:

  - AccountSnapshot in Engine §3.6.1 is renamed ``BrokerAccountSnapshot`` here
    to avoid colliding with ``trading_core.models.AccountSnapshot`` (portfolio
    dimension, already widely consumed). Lead decision (1).
  - PositionLot reuses ``trading_core.models.PositionLot`` (portfolio
    dimension). LocalSim binds to a single portfolio so reuse is natural.
    Lead decision (2).
  - SubscriptionHandle / MarketDataChannel are minimal Pydantic types defined
    here only (broker-layer auxiliary; Codex does not consume). Lead
    decision (3).
  - submit_order_intent is synchronous in LocalSim (fill_callback fires before
    return; OrderHandle.status is already terminal on return). MiniQMTSim will
    be true-async (PR-005). Lead decision (4).

Errors are typed and live in ``trading_core/errors.py``
(``BrokerSubmitError`` / ``BrokerRejectedError`` / ``BrokerConnectivityError``)
— same hierarchy convention as Task #16's ``BrokerMarketSourceMismatchError``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import Callable, Literal

from pydantic import BaseModel, ConfigDict, Field

from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.trading_core.models import OrderIntent, PositionLot


BackendId = Literal["local_sim", "minqmt_sim", "minqmt_live"]


class OrderHandle(BaseModel):
    """Reference returned from submit_order_intent for later cancel / status."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    handle_id: str
    backend_id: BackendId
    submitted_at: datetime
    intent_id: str  # echoes OrderIntent.intent_id


OrderHandleStatusState = Literal[
    "pending",
    "partial_filled",
    "filled",
    "cancelled",
    "rejected",
]


class OrderHandleStatus(BaseModel):
    """Current state snapshot of an OrderHandle (Engine §3.6.1)."""

    model_config = ConfigDict(extra="forbid")

    handle_id: str
    state: OrderHandleStatusState
    filled_quantity: int = Field(ge=0)
    avg_fill_price: Decimal | None = None
    last_event_at: datetime
    rejection_reason: str | None = None  # populated iff state == "rejected"
    raw_status: int | str | None = None
    status_msg: str | None = None
    raw: dict[str, object] = Field(default_factory=dict)


class FillEvent(BaseModel):
    """Fill notification published to subscribed callbacks (Engine §3.6.1)."""

    model_config = ConfigDict(extra="forbid")

    handle_id: str
    intent_id: str
    fill_quantity: int = Field(gt=0)
    fill_price: Decimal
    fill_ts: datetime
    venue: str  # "local_sim" / "minqmt_sim" / ...


class BrokerAccountSnapshot(BaseModel):
    """Broker-side account summary (Engine §3.6.1; renamed from
    AccountSnapshot per Lead 2026-05-08 decision (1))."""

    model_config = ConfigDict(extra="forbid")

    backend_id: BackendId
    cash: Decimal
    nav: Decimal
    margin_used: Decimal | None = None
    as_of: datetime


class CancelAck(BaseModel):
    """Cancel acknowledgement (Engine §3.6.1)."""

    model_config = ConfigDict(extra="forbid")

    handle_id: str
    accepted: bool
    reason: str | None = None


class BrokerBindCapacity(BaseModel):
    """Backend's binding capacity (Engine §3.6.1, R-Q9 D2)."""

    model_config = ConfigDict(extra="forbid")

    backend_id: BackendId
    max_concurrent_packages: int = Field(ge=1)
    rejection_reason_if_exceeded: str


class SubscriptionHandle(BaseModel):
    """Handle returned by subscribe_fill_callback for later unsubscribe.

    Lead 2026-05-08 decision (3): minimal Pydantic shape, no Codex coupling.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    subscription_id: str
    backend_id: BackendId


MarketDataChannelKind = Literal[
    "in_process_tdx",
    "in_process_db",
    "minqmt_xtdata",
]


class MarketDataChannel(BaseModel):
    """Descriptor of the bound market-data channel.

    Engine §3.6.4: market data is strongly bound to broker_id; this descriptor
    is for audit / DecisionTrace only — there is no business logic on it. Lead
    2026-05-08 decision (3).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    backend_id: BackendId
    source: MinuteDataSource
    channel_kind: MarketDataChannelKind


class BrokerBackend(ABC):
    """Adapter-side broker abstraction (Engine §3.6.1).

    Engine never imports this class. Each adapter (Paper / Live / QE) selects
    a concrete BrokerBackend per portfolio binding; Engine's OrderIntent
    contract MUST stay backend-agnostic.

    Synchronicity:
      - LocalSim: synchronous (submit blocks until terminal status; fill_cb
        fires before return; OrderHandle.status is terminal on return)
      - MiniQMTSim: asynchronous (PR-005); submit returns pending handle and
        fills arrive via callback later. Engine code MUST treat submit as
        potentially async; do not assume LocalSim's synchronous semantics.
    """

    backend_id: BackendId
    backend_version: str

    # ----- Order lifecycle (Engine §3.6.1 派单规范) -----
    @abstractmethod
    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle:
        """Translate Engine OrderIntent into backend-native order.

        Errors are typed (``BrokerSubmitError`` / ``BrokerRejectedError`` /
        ``BrokerConnectivityError``) and propagated to the adapter. Adapter
        MUST NOT catch and retry blindly.
        """

    @abstractmethod
    def cancel(self, handle: OrderHandle) -> CancelAck:
        ...

    @abstractmethod
    def query_status(self, handle: OrderHandle) -> OrderHandleStatus:
        ...

    @abstractmethod
    def subscribe_fill_callback(
        self, cb: Callable[[FillEvent], None]
    ) -> SubscriptionHandle:
        ...

    @abstractmethod
    def unsubscribe_fill_callback(self, handle: SubscriptionHandle) -> None:
        """Release a callback registered via subscribe_fill_callback.

        Engine §3.6.1 implies the inverse exists; pinned here so adapters can
        clean up between sessions without leaking callbacks.
        """

    @abstractmethod
    def query_account(self) -> BrokerAccountSnapshot:
        ...

    @abstractmethod
    def query_positions(self) -> dict[str, PositionLot]:
        """Return {symbol: PositionLot}. PositionLot reuses
        ``trading_core.models.PositionLot`` (portfolio dimension; Lead
        2026-05-08 decision (2))."""

    def query_quote(self, symbol: str) -> dict[str, object] | None:
        """Return best bid/ask quote when the broker can provide one.

        Event-driven execution assets call this optional method. Backends that
        cannot provide L1 quotes must return ``None`` so callers fail fast or
        use an explicit limit-price-derived synthetic quote.
        """

        return None

    # ----- Channel + capacity introspection -----
    @abstractmethod
    def market_data_channel(self) -> MarketDataChannel:
        """Returns the bound market-data channel (Engine §3.6.4 / R-Q9 D3)."""

    @abstractmethod
    def bind_capacity(self) -> BrokerBindCapacity:
        """Whether this backend instance accepts MULTIPLE concurrent
        StrategyPackage bindings or only ONE (Engine §3.6.3 / R-Q9 D2)."""
