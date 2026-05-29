"""Adapter-independent DTOs for vn.py-style execution algorithms.

Derived from vn.py/vnpy_algotrading AlgoTemplate and algo DTO usage at commit
4133987530eb28f3538d1983545d81c4f83d7d59. See attribution.py for license data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4


class VnpyAlgoStatus(str, Enum):
    PAUSED = "paused"
    RUNNING = "running"
    STOPPED = "stopped"
    FINISHED = "finished"


class VnpyDirection(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class VnpyOrderType(str, Enum):
    LIMIT = "LIMIT"


class VnpyActionType(str, Enum):
    SUBMIT = "SUBMIT"
    CANCEL = "CANCEL"
    CANCEL_ALL = "CANCEL_ALL"
    LOG = "LOG"
    FINISH = "FINISH"


@dataclass(frozen=True)
class VnpyTick:
    symbol: str
    datetime: datetime
    bid_price_1: float
    bid_volume_1: int
    ask_price_1: float
    ask_volume_1: int
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VnpyOrderUpdate:
    vt_orderid: str
    active: bool
    traded: int = 0
    price: float | None = None
    raw_status: str | None = None
    status_msg: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    raw: dict[str, Any] = field(default_factory=dict)

    def is_active(self) -> bool:
        return bool(self.active)


@dataclass(frozen=True)
class VnpyTradeUpdate:
    vt_orderid: str
    volume: int
    price: float
    trade_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VnpyAction:
    action_type: VnpyActionType
    action_id: str = field(default_factory=lambda: f"vact_{uuid4().hex}")
    vt_orderid: str | None = None
    direction: VnpyDirection | None = None
    price: float | None = None
    volume: int | None = None
    order_type: VnpyOrderType = VnpyOrderType.LIMIT
    reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VnpyAlgoConfig:
    algo_code: str
    symbol: str
    direction: VnpyDirection
    price: float
    volume: int
    setting: dict[str, Any] = field(default_factory=dict)
    algo_name: str | None = None
    min_volume: int = 100
    volume_increment: int = 100


@dataclass(frozen=True)
class VnpyAlgoSnapshot:
    algo_name: str
    algo_code: str
    symbol: str
    direction: str
    price: float
    volume: int
    status: str
    traded: int
    left: int
    traded_price: float
    active_order_ids: list[str]
    parameters: dict[str, Any]
    variables: dict[str, Any]
