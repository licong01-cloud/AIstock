from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from backend.services.qmt_strategy_ledger.tca_calculator import (
    QuoteCandidate,
    TcaCalculationError,
    TcaCalculationInput,
    TcaFill,
    calculate_parent_tca,
    estimate_fee_allocations,
    select_mark,
)
from backend.services.qmt_strategy_ledger.tca_models import canonical_tca_manifest_sha256


NOW = datetime(2026, 7, 10, 6, 55, tzinfo=UTC)
DEADLINE = datetime(2026, 7, 10, 7, 0, tzinfo=UTC)


def test_signed_is_golden_buy_sell_mirror_and_direct_decomposition() -> None:
    buy = calculate_parent_tca(_input(side="BUY", prices=("10", "10.1", "10.2", "10.3")))
    sell = calculate_parent_tca(_input(side="SELL", prices=("10", "9.9", "9.8", "9.7")))

    for result in (buy, sell):
        assert result.values["delay_cost_cny"] == Decimal("10.00000000")
        assert result.values["execution_cost_cny"] == Decimal("4.00000000")
        assert result.values["opportunity_cost_cny"] == Decimal("12.00000000")
        assert result.values["decision_is_gross_cny"] == Decimal("26.00000000")
        assert result.values["decision_is_direct_check_gross_cny"] == Decimal("26.00000000")
        assert result.values["decision_calculation_mode"] == "DECOMPOSED"
        assert result.values["invariant_results"]["direct_decomposed_equality"] is True


def test_canonical_manifest_uses_fixed_decimal_and_utc_millisecond_precision() -> None:
    first = canonical_tca_manifest_sha256(
        {"value": Decimal("1.2"), "time": datetime(2026, 7, 10, 7, 0, 0, 123100, tzinfo=UTC)}
    )
    second = canonical_tca_manifest_sha256(
        {"value": Decimal("1.200000000"), "time": datetime(2026, 7, 10, 7, 0, 0, 123900, tzinfo=UTC)}
    )

    assert first == second


def test_scale_invariance_and_fill_permutation_exact_duplicate_hash() -> None:
    base_input = _input(side="BUY", prices=("10", "10.1", "10.2", "10.3"))
    base = calculate_parent_tca(base_input)
    duplicate = base_input.fills[0]
    permuted = calculate_parent_tca(
        TcaCalculationInput(
            **{
                **_input_kwargs(base_input),
                "fills": (duplicate, duplicate),
            }
        )
    )
    scaled = calculate_parent_tca(
        _input(side="BUY", prices=("10", "10.1", "10.2", "10.3"), scale=2)
    )

    assert permuted.canonical_input_sha256 == base.canonical_input_sha256
    assert permuted.canonical_output_sha256 == base.canonical_output_sha256
    assert scaled.values["decision_is_gross_bps"] == base.values["decision_is_gross_bps"]
    assert scaled.values["decision_is_gross_cny"] == base.values["decision_is_gross_cny"] * 2


def test_same_trade_key_with_different_canonical_fact_is_loud() -> None:
    input_ = _input(side="BUY", prices=("10", "10.1", "10.2", "10.3"))
    conflicting = TcaFill(
        trade_id="trade-1",
        order_id="order-1",
        price=Decimal("10.21"),
        quantity=40,
        trade_time=DEADLINE - timedelta(minutes=1),
        canonical_fact_sha256="b" * 64,
    )
    with pytest.raises(TcaCalculationError, match="ADAPTIVE_IS_TCA_CANONICAL_TRADE_CONFLICT"):
        calculate_parent_tca(TcaCalculationInput(**{**_input_kwargs(input_), "fills": (*input_.fills, conflicting)}))


def test_missing_mark_keeps_opportunity_and_total_null_not_zero() -> None:
    input_ = _input(side="BUY", prices=("10", "10.1", "10.2", "10.3"))
    result = calculate_parent_tca(TcaCalculationInput(**{**_input_kwargs(input_), "deadline_mark": None}))

    assert result.values["opportunity_cost_cny"] is None
    assert result.values["decision_is_gross_cny"] is None
    assert result.values["arrival_is_gross_cny"] is None
    assert result.values["metric_validity"]["decision"]["valid"] is False


def test_arrival_missing_uses_direct_decision_mode_without_fake_components() -> None:
    input_ = _input(side="BUY", prices=("10", "10.1", "10.2", "10.3"))
    result = calculate_parent_tca(TcaCalculationInput(**{**_input_kwargs(input_), "arrival_price": None}))

    assert result.values["decision_calculation_mode"] == "DIRECT"
    assert result.values["decision_is_gross_cny"] == Decimal("26.00000000")
    assert result.values["delay_cost_cny"] is None
    assert result.values["execution_cost_cny"] is None
    assert result.values["arrival_is_gross_cny"] is None


def test_overfill_is_invalid_and_residual_is_not_clamped() -> None:
    input_ = _input(side="BUY", prices=("10", "10.1", "10.2", "10.3"))
    overfill = TcaFill(
        trade_id="trade-2",
        order_id="order-2",
        price=Decimal("10.2"),
        quantity=70,
        trade_time=DEADLINE - timedelta(seconds=1),
        canonical_fact_sha256="c" * 64,
    )
    result = calculate_parent_tca(TcaCalculationInput(**{**_input_kwargs(input_), "fills": (*input_.fills, overfill)}))

    assert result.values["result_status"] == "INVALID"
    assert result.values["deadline_residual_quantity"] is None
    assert result.values["invariant_results"]["deadline_overfill"] is True


def test_cross_trade_date_fill_is_invalid_and_never_enters_terminal_quantity() -> None:
    input_ = _input(side="BUY", prices=("10", "10.1", "10.2", "10.3"))
    cross_day = TcaFill(
        **{
            **{name: getattr(input_.fills[0], name) for name in input_.fills[0].__dataclass_fields__},
            "trade_time": datetime(2026, 7, 11, 1, 31, tzinfo=UTC),
        }
    )
    result = calculate_parent_tca(
        TcaCalculationInput(
            **{
                **_input_kwargs(input_),
                "snapshot_kind": "RECONCILED_FINAL",
                "terminal_as_of": datetime(2026, 7, 11, 2, 0, tzinfo=UTC),
                "fills": (cross_day,),
            }
        )
    )

    assert result.values["result_status"] == "INVALID"
    assert result.values["deadline_filled_quantity"] == 0
    assert result.values["terminal_filled_quantity"] == 0
    assert result.values["invariant_results"]["cross_trade_date_trade_ids"] == ["trade-1"]


def test_mark_selector_is_directional_and_rejects_future_stale_crossed() -> None:
    before = _quote("before", DEADLINE - timedelta(seconds=2), "10.00", "10.02")
    future = _quote("future", DEADLINE + timedelta(seconds=1), "10.02", "10.04")
    deadline_mark = select_mark(
        candidates=(future, before),
        symbol="000001.SZ",
        mark_type="DEADLINE",
        target_time=DEADLINE,
        max_distance_ms=10_000,
        trade_date=date(2026, 7, 10),
    )
    markout = select_mark(
        candidates=(before, future),
        symbol="000001.SZ",
        mark_type="FILL_MARKOUT_60S",
        target_time=DEADLINE,
        max_distance_ms=10_000,
        trade_date=date(2026, 7, 10),
    )

    assert deadline_mark.candidate is before
    assert deadline_mark.mid_price == Decimal("10.01")
    assert markout.candidate is future
    assert markout.mid_price == Decimal("10.03")
    stale = select_mark(
        candidates=(before,),
        symbol="000001.SZ",
        mark_type="DEADLINE",
        target_time=DEADLINE,
        max_distance_ms=1_000,
        trade_date=date(2026, 7, 10),
    )
    assert stale.quality == "STALE"
    assert stale.mid_price is None
    clock_skew = select_mark(
        candidates=(
            QuoteCandidate(
                evidence_id="skew",
                symbol="000001.SZ",
                market_time=DEADLINE - timedelta(seconds=1),
                received_at=DEADLINE - timedelta(seconds=3),
                bid_price_1=Decimal("10"),
                ask_price_1=Decimal("10.02"),
            ),
        ),
        symbol="000001.SZ",
        mark_type="DEADLINE",
        target_time=DEADLINE,
        max_distance_ms=10_000,
        trade_date=date(2026, 7, 10),
        clock_skew_tolerance_ms=1_000,
    )
    assert clock_skew.quality == "CLOCK_SKEW"
    assert clock_skew.mid_price is None


def test_fee_policy_rounds_components_then_stably_allocates_order_minimum() -> None:
    fills = (
        _fill("trade-a", "order-1", "50", 100, DEADLINE - timedelta(minutes=2)),
        _fill("trade-b", "order-1", "50", 100, DEADLINE + timedelta(minutes=1)),
    )
    policy = _fee_policy()
    forward = estimate_fee_allocations(fills, "BUY", policy)
    reverse = estimate_fee_allocations(tuple(reversed(fills)), "BUY", policy)

    assert forward.total_cny == Decimal("5.01")
    assert forward.breakdown == {
        "commission": Decimal("5.00"),
        "exchange_handling": Decimal("0.01"),
        "other": Decimal("0.00"),
        "stamp_tax": Decimal("0"),
        "transfer": Decimal("0.00"),
    }
    assert forward.by_trade == reverse.by_trade
    assert sum((sum(item.values()) for item in forward.by_trade.values()), Decimal(0)) == forward.total_cny


def test_fee_missing_component_contract_stays_missing() -> None:
    policy = _fee_policy()
    del policy["components"]["commission"]["rounding_stage"]
    allocation = estimate_fee_allocations((_fill("trade-a", "order-1", "10", 100, NOW),), "BUY", policy)

    assert allocation.total_cny is None
    assert allocation.quality == "MISSING"
    assert allocation.reason_code == "ADAPTIVE_IS_TCA_FEE_COMPONENT_RULE_INCOMPLETE"


def test_post_deadline_fill_is_lifecycle_only_and_does_not_reduce_deadline_residual() -> None:
    input_ = _input(side="BUY", prices=("10", "10.1", "10.2", "10.3"))
    deadline_fill = TcaFill(
        **{
            **{name: getattr(input_.fills[0], name) for name in input_.fills[0].__dataclass_fields__},
            "actual_fee_cny": Decimal("1"),
            "fee_provenance": "ACTUAL",
            "actual_fee_scope": "TRADE_LEVEL",
        }
    )
    post_fill = TcaFill(
        trade_id="trade-2",
        order_id="order-1",
        price=Decimal("10.4"),
        quantity=60,
        trade_time=DEADLINE + timedelta(minutes=1),
        canonical_fact_sha256="b" * 64,
        actual_fee_cny=Decimal("2"),
        fee_provenance="ACTUAL",
        actual_fee_scope="TRADE_LEVEL",
    )
    result = calculate_parent_tca(
        TcaCalculationInput(
            **{
                **_input_kwargs(input_),
                "snapshot_kind": "RECONCILED_FINAL",
                "terminal_as_of": DEADLINE + timedelta(minutes=5),
                "reconciliation_run_id": "recon-1",
                "finality_satisfied": True,
                "fills": (deadline_fill, post_fill),
                "estimated_fee_policy": _fee_policy(),
            }
        )
    )

    assert result.values["deadline_filled_quantity"] == 40
    assert result.values["terminal_filled_quantity"] == 100
    assert result.values["deadline_residual_quantity"] == 60
    assert result.values["terminal_residual_quantity"] == 0
    assert result.values["post_deadline_execution_cost_cny"] == Decimal("6.00000000")
    assert result.values["deadline_fee_actual_cny"] == Decimal("1.00000000")
    assert result.values["post_deadline_fee_actual_cny"] == Decimal("2.00000000")
    assert (
        result.values["deadline_fee_estimated_cny"]
        + result.values["post_deadline_fee_estimated_cny"]
        == Decimal("5.00000000")
    )


def test_order_level_actual_fee_allocates_once_across_deadline_cutoff() -> None:
    input_ = _input(side="BUY", prices=("10", "10.1", "10.2", "10.3"))
    before = TcaFill(
        trade_id="trade-1",
        order_id="order-1",
        price=Decimal("10"),
        quantity=40,
        trade_time=DEADLINE - timedelta(minutes=1),
        canonical_fact_sha256="a" * 64,
        actual_fee_cny=Decimal("5"),
        fee_provenance="ACTUAL",
        actual_fee_scope="ORDER_LEVEL",
    )
    after = TcaFill(
        trade_id="trade-2",
        order_id="order-1",
        price=Decimal("10"),
        quantity=60,
        trade_time=DEADLINE + timedelta(minutes=1),
        canonical_fact_sha256="b" * 64,
        actual_fee_cny=Decimal("5"),
        fee_provenance="ACTUAL",
        actual_fee_scope="ORDER_LEVEL",
    )
    result = calculate_parent_tca(
        TcaCalculationInput(
            **{
                **_input_kwargs(input_),
                "snapshot_kind": "RECONCILED_FINAL",
                "terminal_as_of": DEADLINE + timedelta(minutes=5),
                "fills": (before, after),
            }
        )
    )

    assert result.values["deadline_fee_actual_cny"] == Decimal("2.00000000")
    assert result.values["post_deadline_fee_actual_cny"] == Decimal("3.00000000")
    assert result.values["deadline_fee_actual_cny"] + result.values["post_deadline_fee_actual_cny"] == Decimal("5.00000000")


def test_partial_effective_spread_never_impersonates_headline() -> None:
    input_ = _input(side="BUY", prices=("10", "10.1", "10.2", "10.3"))
    first = TcaFill(
        **{
            **{name: getattr(input_.fills[0], name) for name in input_.fills[0].__dataclass_fields__},
            "quantity": 50,
            "child_receipt_mid": Decimal("10.1"),
        }
    )
    second = TcaFill(
        trade_id="trade-2",
        order_id="order-1",
        price=Decimal("10.2"),
        quantity=50,
        trade_time=DEADLINE - timedelta(seconds=1),
        canonical_fact_sha256="b" * 64,
    )
    result = calculate_parent_tca(TcaCalculationInput(**{**_input_kwargs(input_), "fills": (first, second)}))

    assert result.values["effective_spread_bps"] is None
    assert result.values["effective_spread_partial_bps"] is not None
    assert result.values["effective_spread_coverage_notional_ratio"] == Decimal("0.500000000000")


def _input(*, side: str, prices: tuple[str, str, str, str], scale: int = 1) -> TcaCalculationInput:
    decision, arrival, execution, mark = map(Decimal, prices)
    selected = select_mark(
        candidates=(_quote("deadline", DEADLINE - timedelta(seconds=1), str(mark), str(mark)),),
        symbol="000001.SZ",
        mark_type="DEADLINE",
        target_time=DEADLINE,
        max_distance_ms=10_000,
        trade_date=date(2026, 7, 10),
    )
    return TcaCalculationInput(
        parent_intent_id="parent-1",
        trade_date=date(2026, 7, 10),
        side=side,
        eligible_quantity=100 * scale,
        decision_price=decision,
        arrival_price=arrival,
        deadline=DEADLINE,
        as_of_time=DEADLINE,
        snapshot_kind="DEADLINE",
        fills=(
            TcaFill(
                trade_id="trade-1",
                order_id="order-1",
                price=execution,
                quantity=40 * scale,
                trade_time=DEADLINE - timedelta(minutes=1),
                canonical_fact_sha256="a" * 64,
            ),
        ),
        deadline_mark=selected,
    )


def _input_kwargs(input_: TcaCalculationInput) -> dict[str, object]:
    return {name: getattr(input_, name) for name in input_.__dataclass_fields__}


def _quote(evidence_id: str, market_time: datetime, bid: str, ask: str) -> QuoteCandidate:
    return QuoteCandidate(
        evidence_id=evidence_id,
        symbol="000001.SZ",
        market_time=market_time,
        received_at=market_time + timedelta(milliseconds=20),
        bid_price_1=Decimal(bid),
        ask_price_1=Decimal(ask),
        raw_quote_sha256="d" * 64,
    )


def _fill(trade_id: str, order_id: str, price: str, quantity: int, trade_time: datetime) -> TcaFill:
    return TcaFill(
        trade_id=trade_id,
        order_id=order_id,
        price=Decimal(price),
        quantity=quantity,
        trade_time=trade_time,
        canonical_fact_sha256=("a" if trade_id.endswith("a") else "b") * 64,
    )


def _fee_policy() -> dict[str, object]:
    base_rule = {
        "calculation_scope": "ORDER",
        "rate_base": "NOTIONAL",
        "minimum_rule": "NONE",
        "minimum_amount": "0",
        "rounding_stage": "COMPONENT_TOTAL",
        "rounding_unit": "0.01",
        "rounding_mode": "ROUND_HALF_UP",
        "applies_to_sides": ["BUY", "SELL"],
    }
    return {
        "fee_schedule_id": "cn-a-v1",
        "effective_from": "2026-01-01",
        "market": "CN_A",
        "account_fee_profile_version": "sim-v1",
        "account_fee_profile_sha256": "e" * 64,
        "fee_allocation_version": "largest-remainder-v1",
        "settlement_rounding": "ROUND_HALF_UP",
        "components": {
            "commission": {**base_rule, "rate": "0.0001", "minimum_rule": "MINIMUM", "minimum_amount": "5"},
            "exchange_handling": {**base_rule, "rate": "0.0000005"},
            "transfer": {**base_rule, "rate": "0"},
            "stamp_tax": {**base_rule, "rate": "0.001", "applies_to_sides": ["SELL"]},
            "other": {**base_rule, "rate": "0"},
        },
    }
