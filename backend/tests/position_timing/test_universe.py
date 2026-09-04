from decimal import Decimal

def test_holding_wins_dedup_and_exited_watchlist_is_excluded(service_factory) -> None:
    service = service_factory()
    payload = service.list_intents()
    assert [item["canonical_symbol"] for item in payload["items"]] == ["000001.SZ", "600000.SH"]
    holding = payload["items"][0]
    assert holding["primary_source_role"].value == "HOLDING"
    assert [role.value for role in holding["source_roles"]] == ["HOLDING", "WATCHLIST"]
    assert holding["pre_action_qty"] == 1000
    assert holding["analysis_effective"] is True
    assert holding["analysis_locked"] is True
    assert holding["analysis_reason_code"] == "HOLDING_ALWAYS_INCLUDED"
    watchlist = payload["items"][1]
    assert watchlist["analysis_selected"] is False
    assert watchlist["analysis_effective"] is False
    assert watchlist["analysis_reason_code"] == "NOT_SELECTED"


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


def test_watchlist_analysis_scope_is_explicit_idempotent_and_next_card_only(service_factory) -> None:
    service = service_factory()
    before = service.list_intents()
    assert before["analysis_scope"].selected_watchlist_symbols == ()
    assert not service.store.root.exists()

    first = service.put_analysis_scope(raw_symbol="600000", analysis_enabled=True)
    second = service.put_analysis_scope(raw_symbol="600000.SH", analysis_enabled=True)
    assert first["status"] == "UPDATED"
    assert first["effective_card_policy"] == "NEXT_CARD_SET_ONLY"
    assert second["status"] == "UNCHANGED"
    assert second["scope_sha256"] == first["scope_sha256"]

    payload = service.list_intents()
    selected = next(item for item in payload["items"] if item["canonical_symbol"] == "600000.SH")
    assert selected["analysis_selected"] is True
    assert selected["analysis_effective"] is True
    assert payload["scope_warnings"] == []


def test_holding_cannot_be_disabled_and_scope_stays_zero_write(service_factory) -> None:
    service = service_factory()
    result = service.put_analysis_scope(raw_symbol="000001.SZ", analysis_enabled=False)
    assert result["status"] == "UNCHANGED"
    assert result["analysis_reason_code"] == "HOLDING_ALWAYS_INCLUDED"
    assert result["analysis_effective"] is True
    assert not service.store.root.exists()


def test_watchlist_only_discovery_with_empty_scope_is_not_reported_as_source_failure(service_factory) -> None:
    service = service_factory(holdings=[])
    result = service.materialize()
    assert result["status"] == "ANALYSIS_UNIVERSE_EMPTY_NO_NEW_CARD"
    assert result["reason_codes"] == ["ANALYSIS_UNIVERSE_EMPTY"]


def test_selected_watchlist_source_drift_is_visible_and_can_be_removed(service_factory, watchlist_rows) -> None:
    service = service_factory(watchlist=watchlist_rows)
    service.put_analysis_scope(raw_symbol="600000.SH", analysis_enabled=True)
    watchlist_rows[:] = [row for row in watchlist_rows if row["code"] != "600000.SH"]

    payload = service.list_intents()
    assert payload["scope_warnings"] == [
        {"canonical_symbol": "600000.SH", "reason_code": "SELECTED_SOURCE_INELIGIBLE"}
    ]
    removed = service.put_analysis_scope(raw_symbol="600000.SH", analysis_enabled=False)
    assert removed["status"] == "UPDATED"
    assert removed["analysis_effective"] is False
    assert service.list_intents()["scope_warnings"] == []
