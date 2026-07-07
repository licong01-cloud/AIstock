"""MiniQMT event-loop runtime contracts.

These protocols freeze the durable event-loop runtime seams.  MiniQMT SIM no
longer has a compiler comparison route, so production callers must use one
algorithm core, one gateway, and qmt_strategy OMS authority.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from backend.execution_algos.vnpy_style import (
    VnpyAction,
    VnpyAlgoSnapshot,
    VnpyOrderUpdate,
    VnpyTick,
    VnpyTradeUpdate,
)
from backend.services.qmt_strategy_ledger.models import (
    CashLedgerEntry,
    OrderLedgerRecord,
    PositionLotRecord,
    TradeLedgerRecord,
    VirtualAccount,
)

from .gateway import MiniQMTGateway, MiniQMTGatewayEventSource


@runtime_checkable
class MiniQMTVnpyAlgoCoreContract(Protocol):
    """Broker-neutral vn.py-style core contract used by the A event-loop runtime."""

    def start(self) -> list[VnpyAction]:
        ...

    def update_tick(self, tick: VnpyTick) -> list[VnpyAction]:
        ...

    def update_order(self, order: VnpyOrderUpdate) -> list[VnpyAction]:
        ...

    def update_trade(self, trade: VnpyTradeUpdate) -> list[VnpyAction]:
        ...

    def update_timer(self) -> list[VnpyAction]:
        ...

    def get_data(self) -> VnpyAlgoSnapshot:
        ...

    def audit_metadata(self) -> dict[str, Any]:
        ...


@runtime_checkable
class MiniQMTStrategyLedgerOmsContract(Protocol):
    """Durable OMS authority backed by qmt_strategy_ledger facts."""

    def get_virtual_account(self, strategy_id: str) -> VirtualAccount:
        ...

    def upsert_order_ledger(self, order: OrderLedgerRecord) -> OrderLedgerRecord:
        ...

    def get_order_ledger(self, account_id: str, qmt_order_id: str) -> OrderLedgerRecord | None:
        ...

    def list_order_ledger(
        self,
        account_id: str,
        *,
        trade_date: Any | None = None,
        strategy_id: str | None = None,
        open_only: bool = False,
    ) -> list[OrderLedgerRecord]:
        ...

    def upsert_trade_ledger(self, trade: TradeLedgerRecord) -> tuple[TradeLedgerRecord, bool]:
        ...

    def create_position_lot(self, lot: PositionLotRecord) -> PositionLotRecord:
        ...

    def list_position_lots(self, strategy_id: str, symbol: str | None = None) -> list[PositionLotRecord]:
        ...

    def append_cash_entry_once(self, entry: CashLedgerEntry) -> tuple[CashLedgerEntry, bool]:
        ...


MiniQMTGatewayContract = MiniQMTGateway
MiniQMTGatewayEventSourceContract = MiniQMTGatewayEventSource

__all__ = [
    "MiniQMTGatewayEventSourceContract",
    "MiniQMTGatewayContract",
    "MiniQMTStrategyLedgerOmsContract",
    "MiniQMTVnpyAlgoCoreContract",
]
