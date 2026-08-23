from __future__ import annotations

from datetime import date

import pytest

from backend.services.qmt_strategy_ledger.lot_availability import tplus1_unlocked
from backend.services.trading_core.errors import DataUnavailableError


class _FrozenPredicate:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[tuple[date, date]] = []

    def tplus1_unlocked(self, open_date: date, as_of_date: date) -> object:
        self.calls.append((open_date, as_of_date))
        return self.result

    @staticmethod
    def is_trading_day(_trade_date: date) -> bool:
        raise AssertionError("frozen authority must not query a trading-day calendar")

    @staticmethod
    def next_trading_day_after(_trade_date: date) -> date:
        raise AssertionError("frozen authority must not query a successor trading day")


def test_tplus1_uses_frozen_exact_predicate_without_calendar_fallback() -> None:
    open_date = date(2026, 8, 21)
    as_of_date = date(2026, 8, 24)
    authority = _FrozenPredicate(True)

    assert tplus1_unlocked(open_date, as_of_date, authority) is True
    assert authority.calls == [(open_date, as_of_date)]


@pytest.mark.parametrize("malformed", [1, None, "true"])
def test_tplus1_rejects_non_boolean_frozen_authority(malformed: object) -> None:
    with pytest.raises(DataUnavailableError, match="strict boolean"):
        tplus1_unlocked(date(2026, 8, 21), date(2026, 8, 24), _FrozenPredicate(malformed))
