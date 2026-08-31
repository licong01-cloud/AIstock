"""Read-only adapter to the global Trading Calendar Service."""

from __future__ import annotations

from datetime import date
from typing import Any, Callable, Iterator

from backend.db.pg_pool import get_conn
from backend.services.simulation_data.contracts import TradingCalendarSnapshot
from backend.services.trading_calendar_status import TradingCalendarStatusService


ConnFactory = Callable[[], Iterator[Any]]


class TradeCalendarProvider:
    """Compatibility-neutral calendar adapter; it owns no calendar query policy."""

    def __init__(
        self,
        conn_factory: ConnFactory | None = None,
        calendar_service: TradingCalendarStatusService | Any | None = None,
    ) -> None:
        self.conn_factory = conn_factory or get_conn
        self.calendar_service = calendar_service or TradingCalendarStatusService(conn_factory=self.conn_factory)

    def ensure_trading_day(self, trade_date: date) -> None:
        self.calendar_service.ensure_trading_day(trade_date)

    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        return self.calendar_service.list_trading_days(start_date, end_date)

    def latest_trading_day_on_or_before(self, as_of_date: date) -> date | None:
        return self.calendar_service.latest_trading_day_on_or_before(as_of_date)

    def snapshot(self, trade_date: date) -> TradingCalendarSnapshot:
        status = self.calendar_service.status(as_of_date=trade_date)

        def optional_date(field: str) -> date | None:
            value = status.get(field)
            return date.fromisoformat(str(value)) if value else None

        return TradingCalendarSnapshot.build(
            trade_date=trade_date,
            is_trading_day=bool(status.get("is_trading_day")),
            previous_trading_date=optional_date("previous_trading_day"),
            next_trading_date=optional_date("next_trading_day"),
            source=str(status.get("source") or "TradingCalendarStatusService"),
        )
