from decimal import Decimal

from backend.services.position_timing.contracts import TriggerSide, TypedStatus
from backend.services.position_timing.policy import estimate_leg_cost, planned_full_notional_threshold


def test_decimal_thresholds_are_derived_without_double_rounding() -> None:
    assert planned_full_notional_threshold(Decimal("1")) == 58_824
    assert planned_full_notional_threshold(Decimal("0.5")) == 117_648
    assert planned_full_notional_threshold(Decimal("0.25")) == 235_295
    assert Decimal("235295") * Decimal("0.25") * Decimal("0.000085") >= Decimal("5")
    assert Decimal("235294") * Decimal("0.25") * Decimal("0.000085") < Decimal("5")


def test_component_cost_applies_minimum_only_to_net_commission() -> None:
    buy = estimate_leg_cost(
        side=TriggerSide.BUY,
        quantity=100,
        reference_price_raw=Decimal("100"),
        symbol="000001.SZ",
    ).scenarios[0]
    sell = estimate_leg_cost(
        side=TriggerSide.SELL,
        quantity=100,
        reference_price_raw=Decimal("100"),
        symbol="000001.SZ",
        full_exit=True,
    ).scenarios[0]
    assert buy.commission_cny == Decimal("5")
    assert buy.total_cost_cny == Decimal("5.6410000")
    assert sell.total_cost_cny == Decimal("10.6410000")
    assert sell.total_cost_cny - buy.total_cost_cny == Decimal("5.0000000")


def test_one_parent_order_round_trip_matches_frozen_fee_examples() -> None:
    low_buy = estimate_leg_cost(
        side=TriggerSide.BUY,
        quantity=100,
        reference_price_raw=Decimal("50"),
        symbol="000001.SZ",
    ).scenarios[0]
    low_sell = estimate_leg_cost(
        side=TriggerSide.SELL,
        quantity=100,
        reference_price_raw=Decimal("50"),
        symbol="000001.SZ",
        full_exit=True,
    ).scenarios[0]
    assert low_buy.total_cost_cny == Decimal("5.3205000")
    assert low_sell.total_cost_cny == Decimal("7.8205000")
    assert (low_buy.total_cost_cny + low_sell.total_cost_cny) / Decimal("5000") * Decimal("10000") == Decimal(
        "26.2820000"
    )

    high_buy = estimate_leg_cost(
        side=TriggerSide.BUY,
        quantity=1000,
        reference_price_raw=Decimal("100"),
        symbol="000001.SZ",
    ).scenarios[0]
    high_sell = estimate_leg_cost(
        side=TriggerSide.SELL,
        quantity=1000,
        reference_price_raw=Decimal("100"),
        symbol="000001.SZ",
        full_exit=True,
    ).scenarios[0]
    assert high_buy.total_cost_bps == Decimal("1.4910000")
    assert high_sell.total_cost_bps == Decimal("6.4910000")
    assert high_buy.total_cost_bps + high_sell.total_cost_bps == Decimal("7.9820000")


def test_parent_order_sensitivity_preserves_total_legal_quantity() -> None:
    estimate = estimate_leg_cost(
        side=TriggerSide.SELL,
        quantity=250,
        reference_price_raw=Decimal("20"),
        symbol="000001.SZ",
        full_exit=True,
    )
    assert estimate.scenarios[0].parent_order_quantities == (250,)
    assert estimate.scenarios[1].status is TypedStatus.AVAILABLE
    assert sum(estimate.scenarios[1].parent_order_quantities) == 250
    assert estimate.scenarios[2].status is TypedStatus.AVAILABLE
    assert estimate.scenarios[2].parent_order_quantities == (100, 100, 50)
    assert estimate.small_trade_cost_heavy is True


def test_star_full_exit_allows_residual_only_in_final_parent_order() -> None:
    estimate = estimate_leg_cost(
        side=TriggerSide.SELL,
        quantity=350,
        reference_price_raw=Decimal("20"),
        symbol="688001.SH",
        full_exit=True,
    )
    assert estimate.scenarios[1].status is TypedStatus.AVAILABLE
    assert estimate.scenarios[1].parent_order_quantities == (200, 150)
