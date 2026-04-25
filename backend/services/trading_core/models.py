"""Trading Core v2 domain models.

The models are deliberately small and strict. Missing business data should fail
validation instead of being converted into empty or default trading results.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class OrderEventType(str, Enum):
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class OrderIntent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intent_id: str = Field(default_factory=lambda: f"intent_{uuid4().hex}")
    package_id: str
    portfolio_id: str
    symbol: str
    side: OrderSide
    quantity: int = Field(gt=0)
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = Field(default=None, gt=0)
    target_trade_date: date
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def _symbol_required(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("symbol is required")
        return value

    @field_validator("quantity")
    @classmethod
    def _quantity_must_be_round_lot(cls, value: int) -> int:
        if value % 100 != 0:
            raise ValueError("quantity must be a 100-share round lot")
        return value

    @model_validator(mode="after")
    def _limit_price_required_for_limit(self) -> "OrderIntent":
        if self.order_type == OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit_price is required for LIMIT orders")
        return self


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(default_factory=lambda: f"ord_{uuid4().hex}")
    intent_id: str
    package_id: str
    portfolio_id: str
    symbol: str
    side: OrderSide
    quantity: int = Field(gt=0)
    order_type: OrderType
    limit_price: float | None = Field(default=None, gt=0)
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: int = Field(default=0, ge=0)
    avg_fill_price: float | None = Field(default=None, gt=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _filled_cannot_exceed_quantity(self) -> "Order":
        if self.filled_quantity > self.quantity:
            raise ValueError("filled_quantity cannot exceed quantity")
        return self

    @property
    def remaining_quantity(self) -> int:
        return self.quantity - self.filled_quantity


class Fill(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fill_id: str = Field(default_factory=lambda: f"fill_{uuid4().hex}")
    order_id: str
    symbol: str
    side: OrderSide
    quantity: int = Field(gt=0)
    price: float = Field(gt=0)
    trade_time: datetime
    bar_time: datetime | None = None
    reason: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("quantity")
    @classmethod
    def _fill_quantity_round_lot(cls, value: int) -> int:
        if value % 100 != 0:
            raise ValueError("fill quantity must be a 100-share round lot")
        return value


class StepFill(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    side: OrderSide
    quantity: int = Field(gt=0)
    price: float = Field(gt=0)
    bar_time: datetime
    reason: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("quantity")
    @classmethod
    def _step_quantity_round_lot(cls, value: int) -> int:
        if value % 100 != 0:
            raise ValueError("step fill quantity must be a 100-share round lot")
        return value


class OrderEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: f"evt_{uuid4().hex}")
    order_id: str
    event_type: OrderEventType
    event_time: datetime = Field(default_factory=lambda: datetime.now(UTC))
    fill: Fill | None = None
    reason: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class MinuteBar(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    bar_time: datetime
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: int = Field(ge=0)
    amount: float | None = Field(default=None, ge=0)
    vwap: float | None = Field(default=None, gt=0)
    is_suspended: bool = False
    limit_up: float | None = Field(default=None, gt=0)
    limit_down: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _ohlc_is_consistent(self) -> "MinuteBar":
        if self.high < self.low:
            raise ValueError("high cannot be lower than low")
        if not (self.low <= self.open <= self.high):
            raise ValueError("open must be between low and high")
        if not (self.low <= self.close <= self.high):
            raise ValueError("close must be between low and high")
        return self

    def to_algo_bar(self) -> dict[str, Any]:
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
            "vwap": self.vwap or self.close,
            "is_suspended": self.is_suspended,
            "limit_up": self.limit_up,
            "limit_down": self.limit_down,
            "bar_time": self.bar_time,
        }


class AccountSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    portfolio_id: str
    cash: float = Field(ge=0)
    market_value: float = Field(ge=0)
    nav: float = Field(ge=0)
    snapshot_time: datetime


class PositionLot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    portfolio_id: str
    symbol: str
    quantity: int = Field(ge=0)
    available_quantity: int = Field(ge=0)
    avg_cost: float = Field(ge=0)
    trade_date: date
