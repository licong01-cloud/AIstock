from backend.services.position_timing.service import normalize_position_symbol


def test_st_changes_limit_rate_but_does_not_make_card_unavailable(service_factory) -> None:
    service = service_factory()
    card = next(
        item for item in service.materialize()["card_set"].cards if item.canonical_symbol == "600000.SH"
    )
    assert card.st_flag is True
    # The shared v2 rule contains the 2026-07-06 main-board ST 10% change.
    assert str(card.limit_up_raw) == "13.20"
    assert str(card.limit_down_raw) == "10.80"
    assert card.action.value == "WAIT"


def test_bj_and_unknown_symbols_are_typed_unsupported(service_factory) -> None:
    holdings = [
        {"id": 1, "code": "830001", "name": "北交所", "cost_price": 10, "quantity": 100},
        {"id": 2, "code": "broken", "name": "坏代码", "cost_price": 10, "quantity": 100},
    ]
    service = service_factory(holdings=holdings, watchlist=[])
    cards = service.materialize()["card_set"].cards
    assert {card.action.value for card in cards} == {"UNAVAILABLE"}
    assert any("UNSUPPORTED_BJ_FIRST_RELEASE" in card.reason_codes for card in cards)
    assert any("UNSUPPORTED_SYMBOL" in card.reason_codes for card in cards)
    assert normalize_position_symbol("830001") == ("830001.BJ", "UNSUPPORTED_BJ_FIRST_RELEASE")
