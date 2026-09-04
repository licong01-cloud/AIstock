from datetime import datetime
from decimal import Decimal

from conftest import CHINA_TZ


def test_before_close_uses_previous_completed_trading_day(service_factory) -> None:
    service = service_factory(now=datetime(2026, 9, 3, 14, 59, tzinfo=CHINA_TZ))
    result = service.materialize()
    card_set = result["card_set"]
    assert card_set.decision_trade_date.isoformat() == "2026-09-02"
    assert card_set.decision_as_of.isoformat() == "2026-09-02T15:00:00+08:00"
    assert card_set.target_trade_date.isoformat() == "2026-09-03"


def test_issued_card_is_not_rewritten_by_later_intent(service_factory) -> None:
    service = service_factory()
    first = service.materialize()["card_set"]
    service.put_intent(
        raw_symbol="000001.SZ",
        planned_full_notional_cny=Decimal("200000"),
        desired_target_exposure=Decimal("0"),
    )
    second = service.materialize()["card_set"]
    assert second == first


def test_card_expires_at_target_day_close(service_factory) -> None:
    service = service_factory()
    card_set = service.materialize()["card_set"]
    assert all(card.valid_until.isoformat() == "2026-09-04T15:00:00+08:00" for card in card_set.cards)
