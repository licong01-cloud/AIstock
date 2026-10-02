from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from backend.services.simulation_data.contracts import SelectionInputSnapshot, TradingCalendarSnapshot
from backend.services.simulation_data.trading_calendar import TradeCalendarProvider


TRADE_DATE = date(2026, 8, 28)


class _CalendarService:
    def status(self, *, as_of_date: date) -> dict[str, object]:
        assert as_of_date == TRADE_DATE
        return {
            "is_trading_day": True,
            "previous_trading_day": "2026-08-27",
            "next_trading_day": "2026-08-31",
            "source": "market.trading_calendar:file_cache",
        }


def test_calendar_adapter_builds_content_addressed_snapshot() -> None:
    snapshot = TradeCalendarProvider(calendar_service=_CalendarService()).snapshot(TRADE_DATE)

    assert snapshot.is_trading_day is True
    assert snapshot.previous_trading_date == date(2026, 8, 27)
    assert snapshot.next_trading_date == date(2026, 8, 31)
    assert snapshot.snapshot_id.startswith("tcs_")

    payload = snapshot.model_dump(mode="python")
    payload["next_trading_date"] = date(2026, 9, 1)
    with pytest.raises(ValueError, match="does not match content"):
        TradingCalendarSnapshot.model_validate(payload)


def test_selection_input_hash_binds_cutoff_source_and_symbol_set() -> None:
    snapshot = SelectionInputSnapshot.build(
        trade_date=TRADE_DATE,
        cutoff_at=datetime(2026, 8, 28, 9, 10, tzinfo=UTC),
        source="selection.daily_input",
        source_version="v1",
        symbol_set=("600000.SH", "000001.SZ", "600000.SH"),
    )

    assert snapshot.symbol_set == ("000001.SZ", "600000.SH")
    payload = snapshot.model_dump(mode="python")
    payload["source_version"] = "v2"
    with pytest.raises(ValueError, match="does not match content"):
        SelectionInputSnapshot.model_validate(payload)
