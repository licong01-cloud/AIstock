from decimal import Decimal

import pandas as pd

from backend.services.position_timing.contracts import canonical_sha256
from backend.services.position_timing.policy import frozen_price_guard_policy
from backend.services.trading_core.price_guard import PriceGuardContext, evaluate as evaluate_price_guard


def test_default_daily_loader_reads_fallback_only_for_missing_symbols(monkeypatch) -> None:
    from backend.data_service import timescaledb_adapter
    from backend.services.position_timing.service import _default_daily_snapshot_loader

    calls = []

    def fake_fetch(universe, **kwargs):
        calls.append((tuple(universe), kwargs))
        if kwargs.get("bars") == 120:
            index = pd.MultiIndex.from_tuples(
                [(pd.Timestamp("2026-09-02"), "000001.SZ")],
                names=["datetime", "instrument"],
            )
            return pd.DataFrame({"close": [10.0]}, index=index)
        index = pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2026-09-03"), "600000.SH")],
            names=["datetime", "instrument"],
        )
        return pd.DataFrame(
            {
                "open": [12.0],
                "high": [12.2],
                "low": [11.8],
                "close": [12.0],
                "volume": [1000.0],
                "amount": [12000.0],
            },
            index=index,
        )

    monkeypatch.setattr(timescaledb_adapter, "fetch_history_window_ts", fake_fetch)
    payload = _default_daily_snapshot_loader(["000001.SZ", "600000.SH"], pd.Timestamp("2026-09-03").date())
    assert len(calls) == 2
    assert calls[1][0] == ("000001.SZ",)
    assert payload["rows"]["600000.SH"]["reference_state"] == "DECISION_DAY_CLOSE"
    assert payload["rows"]["000001.SZ"] == {
        "symbol": "000001.SZ",
        "trade_date": "2026-09-02",
        "close": 10.0,
        "price_basis": "raw_cny",
        "reference_state": "LAST_EXECUTABLE_CLOSE",
        "feature_available_at": "2026-09-02T15:00:00+08:00",
    }


def test_hard_stop_generates_at_open_exit_and_watchlist_without_sizing_waits(service_factory) -> None:
    service = service_factory()
    card_set = service.materialize()["card_set"]
    by_symbol = {card.canonical_symbol: card for card in card_set.cards}
    holding = by_symbol["000001.SZ"]
    watchlist = by_symbol["600000.SH"]
    assert holding.action.value == "EXIT"
    assert holding.execution_window.value == "AT_OPEN"
    assert holding.requested_delta_qty == -1000
    assert holding.triggers[0].branch == "RISK_EXIT_AT_OPEN"
    assert "STOP_LOSS_TRIGGERED" in holding.reason_codes
    assert watchlist.action.value == "WAIT"
    assert "SIZING_INPUT_UNAVAILABLE" in watchlist.reason_codes
    assert watchlist.st_flag is True
    assert holding.adjustment_identity["status"] == "NOT_APPLICABLE"
    assert holding.adjustment_identity["reason_code"] == "BLOCK_ONE_CARD_USES_RAW_PRICE_ONLY"
    assert holding.position_snapshot_sha256 != watchlist.position_snapshot_sha256
    assert holding.intent_snapshot_sha256 != watchlist.intent_snapshot_sha256


def test_non_risk_holding_without_intent_has_an_explicit_hold_reason(service_factory, holding_rows) -> None:
    holdings = [{**holding_rows[0], "cost_price": 9.0}]
    service = service_factory(holdings=holdings)
    holding = next(
        card for card in service.materialize()["card_set"].cards if card.canonical_symbol == "000001.SZ"
    )
    assert holding.action.value == "HOLD"
    assert "RISK_GUARD_HOLD_CURRENT_POSITION" in holding.reason_codes


def test_decision_day_suspension_does_not_erase_target_day_risk_exit(service_factory) -> None:
    from conftest import daily_loader, supporting_loader

    def holding_suspended(symbols, trade_date):
        payload = supporting_loader(symbols, trade_date)
        payload["suspend_facts"]["000001.SZ"]["is_suspended"] = True
        identity = dict(payload["identity"])
        identity["suspend_facts_sha256"] = canonical_sha256(payload["suspend_facts"])
        identity.pop("identity_sha256")
        identity["identity_sha256"] = canonical_sha256(identity)
        payload["identity"] = identity
        return payload

    def holding_last_executable_close(symbols, trade_date):
        payload = daily_loader(symbols, trade_date)
        payload["rows"]["000001.SZ"].update(
            {
                "trade_date": "2026-09-02",
                "reference_state": "LAST_EXECUTABLE_CLOSE",
                "feature_available_at": "2026-09-02T15:00:00+08:00",
            }
        )
        identity = dict(payload["identity"])
        identity["rows_sha256"] = canonical_sha256(payload["rows"])
        identity.pop("identity_sha256")
        identity["identity_sha256"] = canonical_sha256(identity)
        payload["identity"] = identity
        return payload

    service = service_factory(supporting=holding_suspended, daily=holding_last_executable_close)
    holding = next(
        card for card in service.materialize()["card_set"].cards if card.canonical_symbol == "000001.SZ"
    )
    assert holding.action.value == "EXIT"
    assert holding.execution_window.value == "AT_OPEN"
    assert "STOP_LOSS_TRIGGERED" in holding.reason_codes
    assert "DECISION_DAY_SUSPENDED_TARGET_DAY_RECHECK" in holding.reason_codes
    assert "DECISION_DAY_SUSPENDED_USING_LAST_EXECUTABLE_CLOSE" in holding.reason_codes


def test_stale_daily_fallback_without_suspension_does_not_freeze_unavailable_cards(service_factory) -> None:
    from conftest import daily_loader

    def stale_rows(symbols, trade_date):
        payload = daily_loader(symbols, trade_date)
        for row in payload["rows"].values():
            row.update(
                {
                    "trade_date": "2026-09-02",
                    "reference_state": "LAST_EXECUTABLE_CLOSE",
                    "feature_available_at": "2026-09-02T15:00:00+08:00",
                }
            )
        identity = dict(payload["identity"])
        identity["rows_sha256"] = canonical_sha256(payload["rows"])
        identity.pop("identity_sha256")
        identity["identity_sha256"] = canonical_sha256(identity)
        payload["identity"] = identity
        return payload

    service = service_factory(daily=stale_rows)
    result = service.materialize()
    assert result["status"] == "SOURCE_NOT_MATURE_NO_NEW_CARD"
    assert "DAILY_BAR_SOURCE_ZERO_COVERAGE" in result["reason_codes"]
    assert service.store.event_counts() == {}


def test_watchlist_intent_generates_only_frozen_buy_guard_branches(service_factory) -> None:
    service = service_factory()
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("120000"),
        desired_target_exposure=Decimal("1"),
    )
    card_set = service.materialize()["card_set"]
    card = next(item for item in card_set.cards if item.canonical_symbol == "600000.SH")
    assert card.action.value == "OPEN"
    assert card.execution_window.value == "ON_PRICE_TRIGGER"
    assert [trigger.guard_action for trigger in card.triggers] == ["ACCEPT", "REDUCE", "SKIP"]
    assert card.triggers[0].trigger_price_raw < card.triggers[1].trigger_price_raw
    assert card.triggers[0].conditions["price_guard_action"] == "ACCEPT"
    assert card.triggers[1].conditions["price_guard_action"] == "REDUCE"
    assert set(card.triggers[1].conditions["price_guard_reason_codes"]) == {
        "REDUCE_YELLOW_OPEN_GAP",
        "REDUCE_YELLOW_CHASE_BAND",
    }
    assert card.triggers[0].planned_delta_qty > card.triggers[1].planned_delta_qty >= 0
    assert card.triggers[2].planned_delta_qty == 0
    assert set(card.trigger_cost_estimates) == {card.triggers[0].trigger_id, card.triggers[1].trigger_id}
    assert card.trigger_cost_estimates[card.triggers[1].trigger_id].quantity == card.triggers[1].planned_delta_qty
    for trigger in card.triggers[:2]:
        estimate = card.trigger_cost_estimates[trigger.trigger_id]
        assert estimate.reference_price_raw == trigger.trigger_price_raw
        assert trigger.planned_leg_notional_cny == trigger.trigger_price_raw * abs(trigger.planned_delta_qty)
    assert card.hmm_context_status.value == "UNAVAILABLE"
    assert card.action.value != "UNAVAILABLE"


def test_buy_trigger_bands_match_shared_price_guard_actions(service_factory) -> None:
    service = service_factory()
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("120000"),
        desired_target_exposure=Decimal("1"),
    )
    card = next(
        item for item in service.materialize()["card_set"].cards if item.canonical_symbol == "600000.SH"
    )
    green, yellow, skip = card.triggers
    common = {
        "signal_ref_price": float(card.reference_price_raw),
        "prev_close": float(card.reference_price_raw),
        "limit_up": float(card.limit_up_raw),
        "limit_down": float(card.limit_down_raw),
        "side": "buy",
        "price_basis": "raw",
    }
    green_decision = evaluate_price_guard(
        PriceGuardContext(current_price=float(green.trigger_price_raw), **common),
        frozen_price_guard_policy(),
    )
    yellow_probe = (green.trigger_price_raw + yellow.trigger_price_raw) / Decimal("2")
    yellow_decision = evaluate_price_guard(
        PriceGuardContext(current_price=float(yellow_probe), **common),
        frozen_price_guard_policy(),
    )
    skip_decision = evaluate_price_guard(
        PriceGuardContext(current_price=float(yellow.trigger_price_raw + Decimal("0.01")), **common),
        frozen_price_guard_policy(),
    )
    assert green_decision.action == green.conditions["price_guard_action"] == "ACCEPT"
    assert yellow_decision.action == yellow.conditions["price_guard_action"] == "REDUCE"
    assert skip_decision.action == skip.conditions["price_guard_action"] == "SKIP"
    assert green_decision.reason_code in green.conditions["price_guard_reason_codes"]
    assert yellow_decision.reason_code in yellow.conditions["price_guard_reason_codes"]
    assert skip_decision.reason_code in skip.conditions["price_guard_reason_codes"]


def test_green_boundary_keeps_exact_tick_when_shared_evaluator_accepts_it(service_factory) -> None:
    from conftest import daily_loader

    def high_price_daily(symbols, trade_date):
        payload = daily_loader(symbols, trade_date)
        row = payload["rows"]["600000.SH"]
        for field in ("open", "high", "low", "close"):
            row[field] = 100.0
        identity = dict(payload["identity"])
        identity["rows_sha256"] = canonical_sha256(payload["rows"])
        identity.pop("identity_sha256")
        identity["identity_sha256"] = canonical_sha256(identity)
        payload["identity"] = identity
        return payload

    service = service_factory(daily=high_price_daily)
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("120000"),
        desired_target_exposure=Decimal("1"),
    )
    card = next(
        item for item in service.materialize()["card_set"].cards if item.canonical_symbol == "600000.SH"
    )
    green = card.triggers[0]
    assert green.trigger_price_raw == Decimal("100.50")
    decision = evaluate_price_guard(
        PriceGuardContext(
            signal_ref_price=100.0,
            prev_close=100.0,
            current_price=100.5,
            limit_up=float(card.limit_up_raw),
            limit_down=float(card.limit_down_raw),
            side="buy",
        ),
        frozen_price_guard_policy(),
    )
    assert decision.action == "ACCEPT"


def test_single_symbol_data_gap_degrades_only_that_card(service_factory) -> None:
    from conftest import daily_loader

    def missing_one(symbols, trade_date):
        payload = daily_loader(symbols, trade_date)
        payload["rows"].pop("600000.SH")
        return payload

    service = service_factory(daily=missing_one)
    cards = service.materialize()["card_set"].cards
    by_symbol = {card.canonical_symbol: card for card in cards}
    assert by_symbol["600000.SH"].action.value == "UNAVAILABLE"
    assert by_symbol["000001.SZ"].action.value == "EXIT"


def test_confirmed_delist_fact_overrides_holding_target_with_exit(service_factory, holding_rows) -> None:
    from conftest import delist_loader

    holdings = [{**holding_rows[0], "cost_price": 9.0}]

    def confirmed_delist(symbols, trade_date):
        payload = delist_loader(symbols, trade_date)
        fact = payload["rows"]["000001.SZ"]
        fact["delist_flag"] = True
        fact["evidence_hash"] = "e" * 64
        return payload

    service = service_factory(holdings=holdings, delist=confirmed_delist)
    cards = {card.canonical_symbol: card for card in service.materialize()["card_set"].cards}
    holding = cards["000001.SZ"]
    assert holding.action.value == "EXIT"
    assert holding.execution_window.value == "AT_OPEN"
    assert holding.delist_flag is True
    assert holding.delist_context_status.value == "AVAILABLE"
    assert "WATCHLIST_EXPIRED" in holding.reason_codes


def test_post_decision_delist_fact_is_fail_closed_for_only_affected_card(service_factory) -> None:
    from conftest import delist_loader

    def future_delist_fact(symbols, trade_date):
        payload = delist_loader(symbols, trade_date)
        payload["rows"]["600000.SH"]["feature_available_at"] = "2026-09-03T15:00:01+08:00"
        return payload

    service = service_factory(delist=future_delist_fact)
    cards = {card.canonical_symbol: card for card in service.materialize()["card_set"].cards}
    assert cards["600000.SH"].action.value == "UNAVAILABLE"
    assert "DELIST_PIT_UNAVAILABLE" in cards["600000.SH"].reason_codes
    assert cards["000001.SZ"].action.value == "EXIT"


def test_confirmed_delist_fact_blocks_watchlist_buy_without_blocking_other_cards(service_factory) -> None:
    from conftest import delist_loader

    def confirmed_delist(symbols, trade_date):
        payload = delist_loader(symbols, trade_date)
        fact = payload["rows"]["600000.SH"]
        fact["delist_flag"] = True
        fact["evidence_hash"] = "e" * 64
        return payload

    service = service_factory(delist=confirmed_delist)
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("120000"),
        desired_target_exposure=Decimal("1"),
    )
    cards = {card.canonical_symbol: card for card in service.materialize()["card_set"].cards}
    watchlist = cards["600000.SH"]
    assert watchlist.action.value == "UNAVAILABLE"
    assert watchlist.delist_flag is True
    assert watchlist.delist_context_status.value == "AVAILABLE"
    assert "CONFIRMED_DELISTING_BUY_UNAVAILABLE" in watchlist.reason_codes
    assert cards["000001.SZ"].action.value == "EXIT"


def test_systemwide_zero_coverage_returns_typed_no_new_card(service_factory) -> None:
    def no_daily_rows(symbols, trade_date):
        return {
            "rows": {},
            "identity": {"source": "fake-daily", "status": "NOT_MATURE", "identity_sha256": "d" * 64},
        }

    service = service_factory(daily=no_daily_rows)
    result = service.materialize()
    assert result["status"] == "SOURCE_NOT_MATURE_NO_NEW_CARD"
    assert result["card_set"] is None
    assert "DAILY_BAR_SOURCE_ZERO_COVERAGE" in result["reason_codes"]
    assert service.store.event_counts() == {}
    assert service.store.get_card_set(decision_trade_date=result["decision_trade_date"]) is None


def test_required_batch_identity_mismatch_returns_typed_no_new_card(service_factory) -> None:
    from conftest import daily_loader

    def tampered_identity(symbols, trade_date):
        payload = daily_loader(symbols, trade_date)
        payload["identity"]["source"] = "tampered-after-hash"
        return payload

    service = service_factory(daily=tampered_identity)
    result = service.materialize()
    assert result["status"] == "SOURCE_NOT_MATURE_NO_NEW_CARD"
    assert "DAILY_BAR_SOURCE_IDENTITY_INVALID" in result["reason_codes"]
    assert result["card_set"] is None
    assert service.store.event_counts() == {}


def test_empty_universe_returns_typed_no_new_card(service_factory) -> None:
    service = service_factory(holdings=[], watchlist=[])
    result = service.materialize()
    assert result["status"] == "UNIVERSE_EMPTY_NO_NEW_CARD"
    assert result["reason_codes"] == ["TIMING_UNIVERSE_EMPTY"]
    assert service.store.event_counts() == {}


def test_positive_intent_below_board_minimum_has_typed_wait_reason(service_factory) -> None:
    service = service_factory()
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("500"),
        desired_target_exposure=Decimal("0.25"),
    )
    cards = {card.canonical_symbol: card for card in service.materialize()["card_set"].cards}
    assert cards["600000.SH"].action.value == "WAIT"
    assert "TARGET_QUANTITY_BELOW_BOARD_MINIMUM" in cards["600000.SH"].reason_codes


def test_yellow_buy_branch_becomes_skip_when_reduced_size_is_below_board_lot(service_factory) -> None:
    service = service_factory()
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("1500"),
        desired_target_exposure=Decimal("1"),
    )
    cards = {card.canonical_symbol: card for card in service.materialize()["card_set"].cards}
    card = cards["600000.SH"]
    yellow = next(trigger for trigger in card.triggers if trigger.branch == "BUY_YELLOW_REDUCE")
    assert card.action.value == "OPEN"
    assert yellow.planned_delta_qty == 0
    assert yellow.guard_action == "SKIP"
    assert yellow.reason_code == "REDUCE_BRANCH_BELOW_BOARD_LOT_SKIP"
    assert yellow.conditions["price_guard_action"] == "REDUCE"
