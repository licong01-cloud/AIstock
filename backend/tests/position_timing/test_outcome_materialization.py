from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from backend.services.position_timing.contracts import (
    PositionTimingMaterializationStateV1,
    canonical_sha256,
)


CHINA_TZ = ZoneInfo("Asia/Shanghai")


def _weekdays(start: date, end: date) -> list[date]:
    values: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            values.append(current)
        current += timedelta(days=1)
    return values


def _outcome_loader(
    *,
    missing: set[date] | None = None,
    price_by_date: dict[date, Decimal] | None = None,
    adjustment_by_date: dict[date, Decimal] | None = None,
):
    missing = missing or set()
    price_by_date = price_by_date or {}
    adjustment_by_date = adjustment_by_date or {}

    def load(symbols: list[str], start_date: date, end_date: date):
        rows = {symbol: {} for symbol in symbols}
        adjustment_rows = {symbol: {} for symbol in symbols}
        for symbol in symbols:
            for trade_date in _weekdays(start_date, end_date):
                if trade_date in missing:
                    continue
                price = price_by_date.get(trade_date, Decimal("9"))
                adjustment = adjustment_by_date.get(trade_date, Decimal("1"))
                rows[symbol][trade_date.isoformat()] = {
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "open": price,
                    "high": price + Decimal("0.2"),
                    "low": price - Decimal("0.2"),
                    "close": price,
                    "volume": 100000,
                    "amount": 1000000,
                    "price_basis": "raw_cny",
                    "is_suspended": False,
                    "adj_factor": adjustment,
                }
                adjustment_rows[symbol][trade_date.isoformat()] = format(adjustment, "f")
        identity = {
            "source": "fake-outcome",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "symbol_set": symbols,
            "rows_sha256": canonical_sha256(rows),
        }
        identity["identity_sha256"] = canonical_sha256(identity)
        adjustment_identity = {
            "source": "fake-adjustment",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "symbol_set": symbols,
            "rows_sha256": canonical_sha256(adjustment_rows),
        }
        adjustment_identity["identity_sha256"] = canonical_sha256(adjustment_identity)
        return {"rows": rows, "identity": identity, "adjustment_identity": adjustment_identity}

    return load


def test_five_horizons_are_materialized_once_with_paired_incremental_paths(service_factory) -> None:
    clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]
    service = service_factory(now=lambda: clock[0], outcome=_outcome_loader())
    first = service.materialize()
    original_card = first["card_set"].cards[0]
    assert first["outcome_materialization_status"] == "NO_DUE_OUTCOMES"
    initial_evidence = service.evidence()["outcome_evidence"]
    assert initial_evidence["coverage_counts"]["pending_derived"] == 5

    clock[0] = datetime(2026, 10, 9, 16, 0, tzinfo=CHINA_TZ)
    second = service.materialize()
    assert second["outcome_materialization_status"] == "OUTCOMES_MATERIALIZED"
    outcomes = [
        event
        for event in service.store.list_events(event_type="OUTCOME_EVALUATED")
        if event["card_id"] == original_card.card_id
    ]
    assert {event["horizon_trading_days"] for event in outcomes} == {1, 3, 5, 10, 20}
    assert all(event["candidate_path"]["path"] == "SELL_THEN_HOLD_CASH" for event in outcomes)
    assert all(event["do_nothing_path"]["path"] == "HOLD_THEN_TERMINAL_SELL" for event in outcomes)
    assert all(event["net_lift_bps"] is not None for event in outcomes)

    before = len(service.store.list_events(event_type="OUTCOME_EVALUATED"))
    retry = service.materialize()
    assert retry["outcome_materialization_status"] == "OUTCOMES_ALREADY_MATERIALIZED"
    assert len(service.store.list_events(event_type="OUTCOME_EVALUATED")) == before
    state = service.store.get_materialization_state()
    assert state.run_status == "COMPLETE"
    assert state.last_successful_materialization_scan_through_trade_date == date(2026, 10, 9)
    evidence = service.evidence()["outcome_evidence"]
    assert evidence["coverage_counts"]["matured"] >= 5
    assert evidence["paired_matured"]["count"] >= 5


def test_h1_buy_terminal_is_deferred_for_t1_lock(service_factory) -> None:
    clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]
    service = service_factory(
        now=lambda: clock[0],
        holdings=[],
        outcome=_outcome_loader(),
    )
    service.put_analysis_scope(raw_symbol="600000.SH", analysis_enabled=True)
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("120000"),
        desired_target_exposure=Decimal("0.5"),
    )
    first = service.materialize()
    card = first["card_set"].cards[0]
    assert card.requested_delta_qty > 0

    clock[0] = datetime(2026, 9, 7, 16, 0, tzinfo=CHINA_TZ)
    service.materialize()
    event = next(
        item
        for item in service.store.list_events(event_type="OUTCOME_EVALUATED")
        if item["card_id"] == card.card_id and item["horizon_trading_days"] == 1
    )
    assert event["maturity_status"].value == "DEFERRED_THEN_MATURED"
    assert event["nominal_terminal_trade_date"] == date(2026, 9, 4)
    assert event["effective_terminal_trade_date"] == date(2026, 9, 7)
    assert event["deferred_trading_days"] == 1
    assert "TERMINAL_T1_LOCKED" in event["reason_codes"]


def test_incomplete_terminal_defer_does_not_advance_success_watermark(service_factory) -> None:
    clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]
    missing = {date(2026, 9, 7), date(2026, 9, 8)}
    service = service_factory(
        now=lambda: clock[0],
        holdings=[],
        outcome=_outcome_loader(missing=missing),
    )
    service.put_analysis_scope(raw_symbol="600000.SH", analysis_enabled=True)
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("120000"),
        desired_target_exposure=Decimal("0.5"),
    )
    service.materialize()
    initial_watermark = service.store.get_materialization_state().last_successful_materialization_scan_through_trade_date
    clock[0] = datetime(2026, 9, 8, 16, 0, tzinfo=CHINA_TZ)
    result = service.materialize()["outcome_materialization"]
    assert result["status"] == "OUTCOME_MATERIALIZATION_PARTIAL"
    assert result["waiting_for_terminal_defer_count"] >= 1
    assert (
        service.store.get_materialization_state().last_successful_materialization_scan_through_trade_date
        == initial_watermark
    )
    evidence = service.evidence()["outcome_evidence"]
    assert evidence["coverage_counts"]["pending_materialization"] >= 1


def test_unavailable_after_five_day_terminal_defer_is_materialized_as_typed_event(service_factory) -> None:
    clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]
    missing = set(_weekdays(date(2026, 9, 7), date(2026, 9, 11)))
    service = service_factory(
        now=lambda: clock[0],
        holdings=[],
        outcome=_outcome_loader(missing=missing),
    )
    service.put_analysis_scope(raw_symbol="600000.SH", analysis_enabled=True)
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("120000"),
        desired_target_exposure=Decimal("0.5"),
    )
    card = service.materialize()["card_set"].cards[0]
    clock[0] = datetime(2026, 9, 11, 16, 0, tzinfo=CHINA_TZ)
    service.materialize()
    event = next(
        item
        for item in service.store.list_events(event_type="OUTCOME_EVALUATED")
        if item["card_id"] == card.card_id and item["horizon_trading_days"] == 1
    )
    assert event["maturity_status"].value == "UNAVAILABLE_AT_HORIZON"
    assert event["deferred_trading_days"] == 5
    assert event["net_lift_bps"] is None
    assert service.evidence()["outcome_evidence"]["coverage_counts"]["unavailable"] >= 1


def test_daily_conservative_fill_uses_smaller_overlapping_buy_branch(service_factory) -> None:
    service = service_factory(holdings=[])
    service.put_analysis_scope(raw_symbol="600000.SH", analysis_enabled=True)
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("120000"),
        desired_target_exposure=Decimal("1"),
    )
    card = service.materialize()["card_set"].cards[0]
    fill = service._select_outcome_fill(
        card=card,
        target_row={
            "open": Decimal("12.20"),
            "high": Decimal("12.30"),
            "low": Decimal("12.00"),
            "close": Decimal("12.10"),
            "is_suspended": False,
        },
    )
    assert fill.selected_trigger is not None
    assert fill.selected_trigger.branch == "BUY_YELLOW_REDUCE"
    assert "INTRADAY_SEQUENCE_UNOBSERVED_CONSERVATIVE_FILL" in fill.reason_codes

    limit_fill = service._select_outcome_fill(
        card=card,
        target_row={
            "open": card.limit_up_raw,
            "high": card.limit_up_raw,
            "low": card.limit_up_raw,
            "close": card.limit_up_raw,
            "is_suspended": False,
        },
    )
    assert limit_fill.status.value == "POLICY_FILL_UNAVAILABLE_EXPIRED"
    assert "TARGET_DAY_ONE_WORD_LIMIT_UP_BUY_UNAVAILABLE" in limit_fill.reason_codes


def test_corporate_action_adjustment_changes_terminal_share_equivalent(service_factory) -> None:
    clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]
    service = service_factory(
        now=lambda: clock[0],
        holdings=[],
        outcome=_outcome_loader(
            adjustment_by_date={date(2026, 9, 4): Decimal("1"), date(2026, 9, 7): Decimal("2")}
        ),
    )
    service.put_analysis_scope(raw_symbol="600000.SH", analysis_enabled=True)
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny=Decimal("120000"),
        desired_target_exposure=Decimal("0.5"),
    )
    card = service.materialize()["card_set"].cards[0]
    clock[0] = datetime(2026, 9, 7, 16, 0, tzinfo=CHINA_TZ)
    service.materialize()
    event = next(
        item
        for item in service.store.list_events(event_type="OUTCOME_EVALUATED")
        if item["card_id"] == card.card_id and item["horizon_trading_days"] == 1
    )
    assert Decimal(str(event["candidate_path"]["terminal_quantity_equivalent"])) == Decimal(
        abs(event["planned_delta_qty"]) * 2
    )
    assert len(event["adjustment_identity_sha256"]) == 64


def test_success_watermark_exposes_missing_materialization_instead_of_pending(service_factory) -> None:
    clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]
    service = service_factory(now=lambda: clock[0])
    service.materialize()
    clock[0] = datetime(2026, 9, 4, 16, 0, tzinfo=CHINA_TZ)
    service.store.put_materialization_state(
        PositionTimingMaterializationStateV1(
            last_successful_materialization_scan_through_trade_date=date(2026, 9, 4),
            last_run_at=clock[0],
            expected_due_count=0,
            accounted_outcome_count=0,
            run_status="COMPLETE",
        )
    )
    coverage = service.evidence()["outcome_evidence"]["coverage_counts"]
    assert coverage["materialization_missing"] == 1
    assert coverage["pending_derived"] == 4
