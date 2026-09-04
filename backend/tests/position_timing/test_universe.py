from decimal import Decimal


def test_holding_wins_dedup_and_exited_watchlist_is_excluded(service_factory) -> None:
    service = service_factory()
    payload = service.list_intents()
    assert [item["canonical_symbol"] for item in payload["items"]] == ["000001.SZ", "600000.SH"]
    holding = payload["items"][0]
    assert holding["primary_source_role"].value == "HOLDING"
    assert [role.value for role in holding["source_roles"]] == ["HOLDING", "WATCHLIST"]
    assert holding["pre_action_qty"] == 1000


def test_intent_is_exact_idempotent_and_scoped_to_universe(service_factory) -> None:
    service = service_factory()
    first, changed = service.put_intent(
        raw_symbol="600000", planned_full_notional_cny=Decimal("100000"), desired_target_exposure=Decimal("0.5")
    )
    second, changed_again = service.put_intent(
        raw_symbol="600000.SH", planned_full_notional_cny=Decimal("100000"), desired_target_exposure=Decimal("0.5")
    )
    assert changed is True
    assert changed_again is False
    assert first == second


def test_holding_rows_are_presented_before_watchlist_only_rows(service_factory) -> None:
    holdings = [
        {"id": 1, "code": "600001.SH", "name": "持仓", "cost_price": 10, "quantity": 100},
    ]
    watchlist = [
        {
            "id": 2,
            "code": "000001.SZ",
            "name": "自选",
            "advisory_enabled": True,
            "lifecycle_status": "CANDIDATE",
        }
    ]
    payload = service_factory(holdings=holdings, watchlist=watchlist).list_intents()
    assert [item["canonical_symbol"] for item in payload["items"]] == ["600001.SH", "000001.SZ"]
